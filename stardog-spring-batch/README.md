Stardog-Spring-Batch
==========

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
_Current Version **4.0.0**_

This is [Spring Framework](http://springsource.org) integration for [Stardog RDF Database](http://stardog.com). This API provides Spring Batch readers/writers for building batch applications with Stardog.

![Stardog](http://stardog.com/img/stardog.png)

## What is it? ##

This API uses the SnarlTemplate and implements the ItemReader and ItemWriter interfaces from the Spring Batch API.  These can be composed with other readers, writers, and processors to create batch pipelines.


## Usage ##

To use Stardog Spring, we recommend, simply add: "com.complexible.stardog:stardog-spring-batch:2.1.3" to your build dependency.

This does depend on installing the Stardog dependencies:

1. Download Stardog from [Stardog](http://stardog.com)
2. Run the bin/mavenInstall script

Once this is in place, you can create applications using a Spring application context.

4. In your Spring application context, create a DataSourceFactoryBean as your datasource, and reference it in a SnarlTemplate bean
5. Inject your SnarlTemplate bean appropriately, e.g. within the ItemReader/Writers


## Development ##

This project is built with Gradle, and can be easily imported into SpringSource Tool Suite or any other Eclipse based IDE.  If you are updating to a new Stardog version, the core stardog-spring batch instructions should be followed first.

1. Clone the git repository
2. Edit build.gradle to point to Stardog/lib folder, this is used for running the embedded server for testing
3. Run gradle build in stardog-spring
4. Pick up the latest jar in build/libs, or alternatively run "gradle install" to install the built jar into your local M2 folder
5. Run build in stardog-spring-batch


## NOTE ##

This framework is in continuous development, please check the [issues](https://github.com/complexible/stardog-spring/issues) page. You're welcome to contribute.

## License

Copyright 2012-2014 Clark & Parsia

Copyright 2012-2014 Al Baker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
