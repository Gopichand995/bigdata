class HashTable:
    def __init__(self):
        self.MAX = 50
        self.arr = [[] for _ in range(self.MAX)]

    def get_hash(self, key):
        hash = 0
        for char in key:
            hash += ord(char)
        return hash % self.MAX

    def __getitem__(self, key):
        h = self.get_hash(key)
        for element in self.arr[h]:
            if element[0] == key:
                return element[1]

    def __setitem__(self, key, val):
        h = self.get_hash(key)
        found = False
        for idx, element in enumerate(self.arr[h]):
            if len(element) == 2 and element[0] == key:
                self.arr[h][idx] = (key, val)
                found = True
                break
        if not found:
            self.arr[h].append((key, val))

    def __delitem__(self, key):
        h = self.get_hash(key)
        print(h)
        for index, element in enumerate(self.arr[h]):
            if element[0] == key:
                del self.arr[h][index]


if __name__ == "__main__":
    t = HashTable()
    t["march 1"] = 1
    t["march 2"] = 2
    t["march 3"] = 3
    t["march 6"] = 20
    t["march 17"] = 120
    del t["march 18"]
    print(t.arr)
