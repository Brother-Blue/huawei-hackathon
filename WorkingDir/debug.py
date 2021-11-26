#### UTIL TO REMOVE ####
from inspect import  getframeinfo, stack
useDebug = True
def debug(message):
    if(useDebug):
        caller = getframeinfo(stack()[1][0])
        print("[Line]:%d -> %s" % (caller.lineno, message)) # python3 syntax print
    else:
        return
#########END###########