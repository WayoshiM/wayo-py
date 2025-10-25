from argparse import ArgumentParser
from itertools import permutations, tee


def rentanalyze(gps):
    # https://docs.python.org/3/library/itertools.html recipes
    def pairwise(iterable):
        "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    # rent_combos = set()
    solutionCount = [0, 0, 0, 0]
    result = []

    for f, mb in {(t[0], sum(t[1:3]), sum(t[3:5]), t[5]): t[1:5] for t in permutations(gps)}.items():
        idx = 3
        while idx and not all(map(lambda f: f[0] < f[1], pairwise(f[: idx + 1]))):
            idx -= 1

        solutionCount[idx] += 1
        if idx == 3:
            result.append(
                f'{f[0]:.2f} < {mb[0]:.2f} + {mb[1]:.2f} = {f[1]:.2f} < {mb[2]:.2f} + {mb[3]:.2f} = {f[2]:.2f} < {f[3]:.2f}'
            )

    result.append('\n%d: ' % sum(solutionCount) + ' | '.join(map(str, solutionCount)))
    return '\n'.join(result)


if __name__ == '__main__':
    rent_parser = ArgumentParser(description='Analyze 6 GP prices in the context of all 180 possible Rent playings')
    rent_parser.add_argument('G', help='GP prices', type=float, nargs=6)
    rent_parser.add_argument('-c', dest='copy', help='Copy to clipboard', default=False, action='store_true')
    rent_options = rent_parser.parse_args()

    s = rentanalyze(rent_options.G)
    print(s)
    if rent_options.copy:
        import pyperclip

        pyperclip.copy(s)
