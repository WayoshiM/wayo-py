from argparse import ArgumentParser
from math import ceil

GROC_LIMIT = 200000


def grocanalyze(gps, min_total=20.0, max_total=21.0):
    gpss = ['{0:.2f}'.format(gp) for gp in gps]
    breaker = '\t'.join(gpss)
    result = [breaker]

    gp1, gp2, gp3, gp4, gp5 = gps
    glc_sol = []
    sol = 0

    try:
        for glc1 in range(ceil(max_total / gp1)):
            running_sum = glc1 * gp1
            glc_sol.append(glc1)
            for glc2 in range(ceil((max_total - running_sum) / gp2)):
                running_sum += glc2 * gp2
                glc_sol.append(glc2)
                for glc3 in range(ceil((max_total - running_sum) / gp3)):
                    running_sum += glc3 * gp3
                    glc_sol.append(glc3)
                    for glc4 in range(ceil((max_total - running_sum) / gp4)):
                        running_sum += glc4 * gp4
                        glc_sol.append(glc4)
                        for glc5 in range(ceil((min_total - running_sum) / gp5), ceil((max_total - running_sum) / gp5)):
                            glc_sol.append(glc5)
                            result.append(
                                '\t'.join(['{0:d}'.format(lc).rjust(len(gpss[i])) for i, lc in enumerate(glc_sol)])
                                + '\t'
                                + '{0:.2f}'.format(running_sum + glc5 * gp5)
                            )
                            sol += 1
                            if sol == GROC_LIMIT:
                                raise ValueError
                            elif not (sol % 50):
                                result.append('\t'.join(gpss))
                            glc_sol.pop()
                        glc_sol.pop()
                        running_sum -= glc4 * gp4
                    glc_sol.pop()
                    running_sum -= glc3 * gp3
                glc_sol.pop()
                running_sum -= glc2 * gp2
            glc_sol.pop()
            running_sum -= glc1 * gp1
    except ValueError:
        pass

    return sol, '\n'.join(result)


if __name__ == '__main__':
    grocery_parser = ArgumentParser(description='Analyze 5 GP prices in the context of all possible Grocery solutions')
    grocery_parser.add_argument('G', help="GP prices", type=float, nargs=5)
    grocery_parser.add_argument('-c', dest='copy', help='Copy to clipboard', default=False, action='store_true')
    grocery_options = grocery_parser.parse_args()

    i, s = grocanalyze(grocery_options.G)
    print(i)
    print(s)
    if grocery_options.copy:
        import pyperclip

        pyperclip.copy(s)
