Stardog-Spring
==========

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)  
_Current Version **2.1.1**_ 

This is [Spring Framework](http://springsource.org) integration for [Stardog RDF Database](http://stardog.com). These bindings
provide Spring aware beans to provide an analogous feature set to Spring's 
jdbcTemplate.  

Spring support sub-projects:
* stardog-spring : Core Spring framework
* stardog-spring-batch: Spring Batch support


![Stardog](http://docs.stardog.com/img/sd.png)    

## What is it? ##

This binding uses the native Stardog SNARL API and creates the appropriate Spring aware beans and template ease of use facilities for idiomatic Spring development.  Like a JDBC DataSource, and a JdbcTemplate, the Stardog Spring provides a DataSource abstraction and a SnarlTemplate abstraction.

The implementation requires the Stardog libraries to run, and therefore can run with:

* Stardog SNARL client/server
* Stardog HTTP client/server
* Stardog embeddded, via DataSoure configuration

For more information, go to the Stardog's [Spring Programming](http://stardog.com/docs/spring/) documentation.

The framework is currently targeted to the core Spring Framework (Spring 3.1.2 as of Stardog 1.1.1).  It has been tested in parts of the larger Spring ecosystem including Spring Web MVC, Grails 1.3, Grails 2.0, Grails 2.2.  It should also work well with other related Spring projects, such as Spring Integration.

## Usage ##

To use Stardog Spring, we recommend:

1. Download Stardog from [Stardog](http://stardog.com)
2. Run the bin/mavenInstall script
3. Add "com.complexible.stardog:stardog-spring:2.1.2" as a dependency in your Gradle, Maven, Ivy file etc
4. In your Spring application context, create a DataSourceFactoryBean as your datasource, and reference it in a SnarlTemplate bean
5. Inject your SnarlTemplate bean appropriately in your application.  It is thread safe.

If desired, you can create a SnarlTemplate programmatically outside of Spring by using a ConnectionConfiguration object from the Stardog API like so:

```groovy
ConnectionConfiguration cc = ConnectionConfiguration
	.to("myDB")
	.credentials("admin", "admin")
	.url("snarl://localhost/")

SnarlTemplate snarlTemplate = new SnarlTemplate()
snarlTemplate.setDataSource(new DataSource(cc))
```


## Development ##

This project is built with Gradle, and can be easily imported into SpringSource Tool Suite or any other Eclipse based IDE. 


1. Clone the git repository
2. Edit build.gradle to point to Stardog/lib folder, this is used for running the embedded server for testing
3. Run gradle build
4. Pick up the latest jar in build/libs, or alternatively run "gradle install" to install the built jar into your local M2 folder

All tests should pass with a working Stardog download, including a license.  Common issues can be seen with "gradle build --debug", where Stardog does exit on invalid licenses.  The exit of the embedded server does cause the Gradle build to exit not so gracefully, so running with --debug for unexpected errors will show what happened.  Likewise, you can take a look at the stardog.log file to see if a license error occured.  If you do not wish to run Emma code coverage tool, remove it from the Gradle configuration


## NOTE ##

This framework is in continuous development, please check the [issues](https://github.com/complexible/stardog-spring/issues) page. You're welcome to contribute.

## License

Copyright 2012-2014 Clark & Parsia, Al Baker

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

* [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)  

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
