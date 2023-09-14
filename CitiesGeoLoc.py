def main(argv):
    cities =""
    if argv : cities = argv[0]
    else: cities = "cities.txt" 
    file_name ="citiesLoc.txt"
    
    newF = open(file_name, 'w')
    f = open(cities,"r")

    for l in f.readlines():
        city = l.strip()
        url = "https://geocode.maps.co/search?q={}".format(city)
        response = requests.get(url)
        json = response.json()[0]
        stri = ' '.join((city,json['lat'],json['lon']))
        newF.write(stri+"\n")
        print(city, "added to citiesLoc.txt file" )

    newF.close(); f.close()

import sys, requests,os

if __name__ == "__main__":
    main(sys.argv[1:])