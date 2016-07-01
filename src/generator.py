"""Base file for generator

"""

def main():
    from generator_scenarios.cdr import cdr_gen
    return cdr_gen()

if __name__ == "__main__":
    main()
