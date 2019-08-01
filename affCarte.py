import folium
from folium.plugins import MarkerCluster
from glob import glob

nbFic=0
for fic in glob('tweets_location*'):
    nbFic+=1
    nbFic2 = nbFic
    nbFic2 = folium.Map(location = [21.027964, 105.851013], zoom_start = 14)
    marker_cluster = MarkerCluster().add_to(nbFic2)
    loc = []
    locF = []
    for ligne in open(fic, 'r'):
        liste = ligne.rstrip('\n\r').split(" ")
        for i in range (len(liste)):
            loc.append((float(liste[i])))
        locF.append(loc)
        loc=[]
    for i in range (0, len(locF)):
        folium.Marker(locF[i]).add_to(marker_cluster)
    nbFic2.save('hanoi_'+str(fic)+'.html')

