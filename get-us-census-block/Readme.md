This custom recipe gets the US Census block id, the block group and the tract id corresponding to the Latitude Longitude provided. 
More info here: https://www.census.gov/geo/reference/

It uses the free geo.fcc.gov's API.

Output:
If the Latitude / Longitude does not exist in the fcc API then no records are returned.

In options:
The BBOX (Bounding Box) returned gets this format [-77.511283, 38.252267, -77.495614, 38.267525]