# package index service

## what is this? 

![raiders-warehouse](raiders-warehouse.jpg)

The package index service is part of the ingest pipeline stage. It serves the following functions:

* retrieves information about one or more packages from a given package index (npm, pypi, maven central, etc). This may include when a given package was published, how many major/minor/patch versions behind head a each package asked after is. 

see [one sheet])https://docs.google.com/document/d/1OpUMP69W9lL42g5tdJxAq8r3x9snth5uOLUIT7rIJ3I/edit?usp=drive_link) for more information. 

## how to a build it? 

`mvn clean install` 

Will build the service into a jar as well as create a docker image. 

## how to I make it go? 

The first thing you need to do is build the project. 

Then you'll need to spin up kafka and postgres. Do so by way of the docker-compose file thusly 

`docker-compose up` 

Then you can spin up the data-service from a new terminal thusly

`mvn spring-boot:run` 

## where can I get more information? 

This is a PatchFox [turbo](https://gitlab.com/patchfox2/turbo) service. Click the link for more information on what that means and what turbo-charged services provide to both developers and consumers. 
