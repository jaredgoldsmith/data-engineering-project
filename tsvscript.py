
#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []

with open('data2.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[2:len(data)-4]:
        line.strip()
        row = line.split("\\t")
        
        # skip the rows where speed is missing
        print(row)
        x = row[0].split('|')
        print(x)
        #y = row[1]
        #speed = row[2]
        x,y,speed = x
        if speed is None or speed == "":
            continue
        try:
            x = float(x)
            y = float(y)
        except:
            continue
        try:
            speed = int(speed)
        except:
            continue
        print(x)
        print(y)
        print(speed)
        if speed is None or speed == "":
            continue
     
        try:
            latitude, longitude = map(float, (y, x))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open("data2.geojson", "w") as f:
    f.write('%s' % collection)
