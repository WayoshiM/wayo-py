import logging
import portion as P

_log = logging.getLogger('wayo_log')

# https://github.com/AlexandreDecan/portion#specialize--customize-intervals

try:

    class SeasonInterval(P.AbstractDiscreteInterval):
        _step = 1

        @classmethod
        def from_atomic(cls, left, lower, upper, right):
            return super().from_atomic(
                P.CLOSED if lower < 1 else left,
                max(1, lower),
                upper,
                right,
            )

    SI = P.create_api(SeasonInterval)
except Exception as e:
    _log.warning('Could not load portion 2.4.0 DiscreteInterval functionality, reverting to regular Portion')
    _log.error(e)
    SI = P
