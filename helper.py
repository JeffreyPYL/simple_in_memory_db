
def checkStr(*args):
        for val in args:
            if type(val) != str:
                return False
        return True