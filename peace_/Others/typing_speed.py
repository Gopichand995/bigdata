from time import time


# calculate the accuracy of the input prompt
def tperror(prompt):
    global inwords
    words = prompt.split()
    errors = 0

    for i in range(len(inwords)):
        if i in (0, len(inwords) - 1):
            if inwords[i] == words[i]:
                continue
            else:
                errors += 1
        else:
            if inwords[i] == words[i]:
                if inwords[i + 1] == words[i + 1] and inwords[i - 1] == words[i - 1]:
                    continue
                else:
                    errors += 1
            else:
                errors += 1
    return errors


# calculate the speed of the typing words per minute
def speed(inprompt):
    global time
    global inwords

    inwords = inprompt.split()
    twords = len(inwords)
    speed = twords / time
    return speed


# calculate the total elapsed time
def elapsedtime(stime, etime):
    return (etime - stime)/60


if __name__ == "__main__":
    prompt = "Python is an interpreted, high-level language, general-purpose programming language. Let us learn " \
             "the basics of this short line code easy language"
    print(f"Type this:- {prompt}")
    input("Press Enter when u r ready")

    stime = time()
    inprompt = input()
    etime = time()

    time = round(elapsedtime(stime, etime), 2)
    speed = speed(inprompt)
    errors = tperror(prompt)

    print(f"Total time elapsed: {time} seconds")
    print(f"Your average Typing Speed was {speed} words per minute")
    print(f"With the total of {errors} errors")
