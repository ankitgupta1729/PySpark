col=[]
count=0
def collatz(n):
    global count
    if n==2 or n==1:
        col.append(1)
        count+=1
    elif n%2==0:
        col.append(n/2)
        collatz(n/2)
        count+=1
    else:
        col.append(3*n+1)
        collatz(3*n+1)
        count+=1
n=int(input("Enter the number: "))
col.append(n)
collatz(n)
print("list: ",col)
print("steps: ",count)