# import numpy as np
import argparse

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
            "--num",
            type=int,
            default=3,
        )
    return parser.parse_args()

def simple_function(x):
    x = x+1
    return x

# print("return x+1: ", simple_function(2))

if __name__ == "__main__":
    print("hello world")
    namespace = parse_args()
    print("input arg: ", namespace.num)
    print("output from arg: ", simple_function(namespace.num))
    
    
