# Geo

These are functions for detecting if the location point is inside the given set of geofences.
The function can be used in the directive `filter-row-if-false`, `filter-row-if-true`, `filter-row-on`,
`set column`, `set-column` or `send-to-error`.

## Pre-requisite

The Geofences should be represented in [geojson](https://geojson.org/) format. The location coordinates should be
represented as Double type values .

## Example data

`fence` in the input is defined as json specified below:

```
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -122.05870628356934,
              37.37943348292772
            ],
            [
              -122.05724716186525,
              37.374727268782294
            ],
            [
              -122.04634666442871,
              37.37493189292912
            ],
            [
              -122.04608917236328,
              37.38175237839049
            ],
            [
              -122.05870628356934,
              37.37943348292772
            ]
          ]
        ]
      }
    }
  ]
}
```

## InFence
Checks if the given coordinate is inside any of the given polygonal geofences based on the winding number algorithm.
If any of the inputs is null, this method will return false

### Namespace
`geo`

### Input
latitude(`double`), longitude(`double`), json fence (`string`)

### Output
true/false

### Example
if `latitude` contains `37.378990156513105` and `longitude` contains `-122.05076694488525` and
`fence` contains the previously mentioned json string, then resulting operation is `true`

```
  set-column infence geo:InFence(37.378990156513105, -122.05076694488525, fence)
  send-to-error !geo:InFence(latitude,longitude,fence)
```

When `latitude` contains `43.46089378008257` and `longitude` contains `-462.49145507812494` and
`fence` contains a json `string`, then resulting operation is `false`

```
  set-column infence geo:InFence(43.46089378008257, -462.49145507812494, fence)
  send-to-error !geo:InFence(latitude,longitude,fence)
```
