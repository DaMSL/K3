#!/usr/bin/env python
import argparse
import six
import random

random.seed()

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--order", type=int, help="10^x numbers",
                        default=6)
    parser.add_argument("--max", type=int, help="max number (default is max_int)",
                        default=3)
    args = parser.parse_args()

    for i in range(pow(10, args.order)):
        six.print_(random.randint(0, args.max))

if __name__ == '__main__':
    main()
