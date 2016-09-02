class T(object):

    def __init__(self, a):
        self.a = a

    @staticmethod
    def build(a):
        return T(a)



b = T.build(10)
print b
print b.a

