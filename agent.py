import os
import sys
import argparse
import pandas as pd
from typing import TypedDict, Dict, Any
from langgraph.graph import StateGraph, END
from groq import Groq
from dotenv import load_dotenv
from prompt import (
    ANALYZE_PROMPT,
    GENERATE_PARSER_PROMPT,
    SELF_CORRECT_PROMPT,
    REFLECTION_PROMPT
)

load_dotenv()

class AgentState(TypedDict):
    target_bank: str
    pdf_path: str
    csv_path: str
    analysis: str
    current_code: str
    error_message: str
    attempt: int
    max_attempts: int
    success: bool
    plan: Dict[str, Any]
    reflection: str

class BankParserAgent:
    def __init__(self):
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.graph = self._create_graph()

    def call_llm(self, prompt: str) -> str:
        try:
            response = self.client.chat.completions.create(
                model="deepseek-r1-distill-llama-70b",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
            return response.choices[0].message.content
        except Exception:
            return ""

    def plan_node(self, state: AgentState) -> AgentState:
        if state['attempt'] == 1 and not state.get('analysis'):
            state['plan'] = {"action": "ANALYZE", "reasoning": ""}
        elif state.get('error_message') and state.get('current_code'):
            state['plan'] = {"action": "CORRECT", "reasoning": ""}
        elif state.get('analysis') and not state.get('current_code'):
            state['plan'] = {"action": "GENERATE", "reasoning": ""}
        else:
            state['plan'] = {"action": "GENERATE", "reasoning": ""}
        return state

    def analyze_node(self, state: AgentState) -> AgentState:
        try:
            df = pd.read_csv(state['csv_path'])
            csv_info = f"""
CSV Structure:
- Columns: {list(df.columns)}
- Shape: {df.shape}
- Data Types: {df.dtypes.to_dict()}
- Sample data:
{df.head(3).to_string()}
{df.iloc[10:13].to_string() if len(df) > 10 else ""}
            """
            prompt = f"{ANALYZE_PROMPT}\n\nCSV Info:\n{csv_info}\nPDF Path: {state['pdf_path']}"
            state['analysis'] = self.call_llm(prompt)
        except Exception as e:
            state['error_message'] = f"Analysis failed: {str(e)}"
        return state

    def generate_node(self, state: AgentState) -> AgentState:
        try:
            df = pd.read_csv(state['csv_path'])
            columns = list(df.columns)
            prompt = GENERATE_PARSER_PROMPT.format(
                analysis=state.get('analysis', ''),
                target_bank=state['target_bank'],
                columns=columns
            )
            code_response = self.call_llm(prompt)
            if "```python" in code_response:
                code = code_response.split("```python")[1].split("```")[0]
            elif "```" in code_response:
                code = code_response.split("```")[1]
            else:
                code = code_response
            state['current_code'] = code.strip()
            self._save_parser(state['target_bank'], state['current_code'])
        except Exception as e:
            state['error_message'] = f"Code generation failed: {str(e)}"
        return state

    def correct_node(self, state: AgentState) -> AgentState:
        try:
            prompt = SELF_CORRECT_PROMPT.format(
                error=state['error_message'],
                code=state['current_code']
            )
            corrected_response = self.call_llm(prompt)
            if "```python" in corrected_response:
                corrected_code = corrected_response.split("```python")[1].split("```")[0]
            elif "```" in corrected_response:
                corrected_code = corrected_response.split("```")[1]
            else:
                corrected_code = corrected_response
            state['current_code'] = corrected_code.strip()
            self._save_parser(state['target_bank'], state['current_code'])
        except Exception as e:
            state['error_message'] = f"Correction failed: {str(e)}"
        return state

    def test_node(self, state: AgentState) -> AgentState:
        try:
            sys.path.append('custom_parsers')
            module_name = f"{state['target_bank']}_parser"
            if module_name in sys.modules:
                del sys.modules[module_name]
            parser_module = __import__(module_name)
            result_df = parser_module.parse(state['pdf_path'])
            expected_df = pd.read_csv(state['csv_path'])

            if result_df.equals(expected_df):
                state['success'] = True
                state['error_message'] = ""
            else:
                state['success'] = False
                details = [
                    f"Got columns: {list(result_df.columns)}",
                    f"Expected columns: {list(expected_df.columns)}",
                    f"Got shape: {result_df.shape}",
                    f"Expected shape: {expected_df.shape}"
                ]
                if list(result_df.columns) == list(expected_df.columns):
                    details.append(f"Got dtypes: {result_df.dtypes.to_dict()}")
                    details.append(f"Expected dtypes: {expected_df.dtypes.to_dict()}")
                    for col in result_df.columns:
                        if col in expected_df.columns:
                            got_val = result_df[col].iloc[0] if len(result_df) > 0 else "N/A"
                            exp_val = expected_df[col].iloc[0] if len(expected_df) > 0 else "N/A"
                            if str(got_val) != str(exp_val):
                                details.append(f"Mismatch in column '{col}': '{got_val}' vs '{exp_val}'")
                state['error_message'] = "\n".join(details)
        except Exception as e:
            state['success'] = False
            state['error_message'] = f"Test execution failed: {str(e)}"
        state['attempt'] += 1
        return state

    def reflect_node(self, state: AgentState) -> AgentState:
        prompt = REFLECTION_PROMPT.format(
            target_bank=state['target_bank'],
            attempts_made=state['attempt'] - 1,
            final_error=state['error_message'],
            final_code=state['current_code']
        )
        state['reflection'] = self.call_llm(prompt)
        return state

    def _save_parser(self, target_bank: str, code: str):
        os.makedirs("custom_parsers", exist_ok=True)
        path = f"custom_parsers/{target_bank}_parser.py"
        with open(path, "w") as f:
            f.write(code)

    def _should_continue(self, state: AgentState) -> str:
        if state['success']:
            return "success"
        elif state['attempt'] > state['max_attempts']:
            return "reflect"
        return "plan"

    def _route_after_plan(self, state: AgentState) -> str:
        action = state['plan']['action']
        if action == "ANALYZE":
            return "analyze"
        elif action == "GENERATE":
            return "generate"
        elif action == "CORRECT":
            return "correct"
        return "generate"

    def _create_graph(self) -> StateGraph:
        workflow = StateGraph(AgentState)
        workflow.add_node("plan", self.plan_node)
        workflow.add_node("analyze", self.analyze_node)
        workflow.add_node("generate", self.generate_node)
        workflow.add_node("correct", self.correct_node)
        workflow.add_node("test", self.test_node)
        workflow.add_node("reflect", self.reflect_node)

        workflow.set_entry_point("plan")
        workflow.add_conditional_edges("plan", self._route_after_plan, {
            "analyze": "analyze",
            "generate": "generate",
            "correct": "correct"
        })

        workflow.add_edge("analyze", "generate")
        workflow.add_edge("generate", "test")
        workflow.add_edge("correct", "test")

        workflow.add_conditional_edges("test", self._should_continue, {
            "success": END,
            "reflect": "reflect",
            "plan": "plan"
        })

        workflow.add_edge("reflect", END)
        return workflow.compile()

    def run(self, target_bank: str) -> bool:
        print("Started agent run")

        pdf_path = f"data/{target_bank}/{target_bank} sample.pdf"
        csv_path = f"data/{target_bank}/result.csv"

        if not os.path.exists(pdf_path):
            return False
        if not os.path.exists(csv_path):
            return False

        initial_state = AgentState(
            target_bank=target_bank,
            pdf_path=pdf_path,
            csv_path=csv_path,
            analysis="",
            current_code="",
            error_message="",
            attempt=1,
            max_attempts=3,
            success=False,
            plan={},
            reflection=""
        )

        try:
            final_state = self.graph.invoke(initial_state)
            print("Finished agent run")
            return final_state['success']
        except Exception:
            return False

def main():
    parser = argparse.ArgumentParser(description="Parser agent")
    parser.add_argument("--target", required=True, help="Target bank name")
    args = parser.parse_args()

    if not os.getenv("GROQ_API_KEY"):
        sys.exit(1)

    agent = BankParserAgent()
    result = agent.run(args.target)
    sys.exit(0 if result else 1)

main()
