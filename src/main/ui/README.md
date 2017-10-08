# ![KairosDB](resources/images/kairosdb_light.png) WebUI

This is an Angular 2 rewrite of the KairosDB query building interface.

It still work in progress but is almost ready for production. 
It requires the complete meta-data support on KairosDB server.


## How to build

Using node and npm.

* If not done already - install npm dependecies using `npm install`
* Then build using `npm run build`
* The application is generated in `dist` folder

You may run the web server with lite for testing `npm run lite`


## How to clean

Use the following snippet to create an alias (using bash):
`alias clean_tsc="find app -name '*.json' -delete && find app -name '*.js' -delete && find app -name '*.map' -delete && find app -name '*.ngfactory.ts' -delete && find app -name '*.shim.*' -delete"`

Then run `clean_tsc` to do a "clean" of the environment (i.e. remove compilation artefacts)

