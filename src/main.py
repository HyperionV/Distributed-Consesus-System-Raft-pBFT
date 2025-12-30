import argparse, sys
from infrastructure.node import Node

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--id', type=int, required=True)
    args = p.parse_args()
    if args.id < 1 or args.id > 5:
        print("Error: ID 1-5")
        sys.exit(1)
    Node(args.id).serve()

if __name__ == '__main__':
    main()
