from functools import wraps
import os


def attach_motherduck(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not os.environ.get("motherduck_token"):
            raise ValueError("Motherduck token has not been set")
        self.execute("ATTACH 'md:'")
        return func(self, *args, **kwargs)

    return wrapper
