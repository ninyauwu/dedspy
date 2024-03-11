from queue import LifoQueue

dic = {
    'Man': 'Vrouw',
    'Zon': 'Maan',
    'Heet': 'Koud',
    'Hard': 'Zacht'
}

print(dic['Man'])

arr = ['Nul', 'Een', 'Twee', 'Drie']
print(arr[2])


# Stack code gekopieerd van https://www.geeksforgeeks.org/reverse-stack-without-using-extra-space/
class StackNode:

    def __init__(self, data):
        self.data = data
        self.next = None


class Stack:

    def __init__(self):

        self.top = None

    # Push and pop operations
    def push(self, data):

        if (self.top == None):
            self.top = StackNode(data)
            return

        s = StackNode(data)
        s.next = self.top
        self.top = s

    def pop(self):

        s = self.top
        self.top = self.top.next
        return s

        # Prints contents of stack

    def display(self):

        s = self.top

        while (s != None):
            print(s.data, end=' ')
            s = s.next

    # Mijn search algoritme
    def search(self, data):
        s = self.top
        while (s != None):
            s = s.next
            if (s == None):
                break
            if (s.data == data):
                break

        return s

    # Get value at index counting from rear in O(n) time
    def get(self, index):
        all = []
        s = self.top
        while (s != None):
            all.append(s)
            s = s.next

        return all[len(all) - 1 - index].data


stack = Stack()
stack.push('Watta')
stack.push('Breggie')
stack.push('Smiggel')
stack.push('Klomstra')
stack.push('De Waal')
stack.push('Manneke')
stack.push('Marianneke')
stack.push('Buikje Eug')
stack.push('Johannes \'De Strijder\' Willems')

print(stack.get(6))
