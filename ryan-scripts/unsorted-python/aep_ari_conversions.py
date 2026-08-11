from math import log, exp, ceil

def AEPfromARI(ARI):
	AEP = (exp(1/ARI)-1)/exp(1/ARI)
	return AEP*100 # 1 in X AEP
def ARIfromAEP(AEP): # 1 in X AEP
	ARI = 1/(-log(1-AEP/100))
	return ARI
def AEP1inXfromARI(ARI):
	EY = 1/ARI
	AEP1inX = exp(EY)/(exp(EY)-1)
	return AEP1inX