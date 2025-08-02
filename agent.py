import argparse
import sys
import os
from graph import BankParserAgent
from dotenv import load_dotenv

def main():
    parser = argparse.ArgumentParser(description="Parser agent")
    parser.add_argument("--target", required=True, help="Target bank name")
    args = parser.parse_args()

    load_dotenv()
    if not os.getenv("GROQ_API_KEY"):
        print("Missing GROQ_API_KEY")
        sys.exit(1)

    agent = BankParserAgent()
    result = agent.run(args.target)
    sys.exit(0 if result else 1)

if __name__ == "__main__":
    main()

