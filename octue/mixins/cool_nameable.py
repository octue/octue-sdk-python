from coolname import generate_slug


class CoolNameable:
    def __init__(self):
        self.cool_name = generate_slug(2)
