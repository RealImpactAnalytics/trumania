"""Base file for generator

"""

def main():
    from generator_scenarios.cdr import generate
    return generate()

if __name__ == "__main__":
    main()
