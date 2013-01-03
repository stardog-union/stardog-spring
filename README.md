Stardog-Spring
==========

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)  
_Current Version **1.1.1**_ 

This is [Spring Framework](http://springsource.org) integration for [Stardog RDF Database](http://stardog.com). These bindings
provide Spring aware beans to provide an analogous feature set to Spring's 
jdbcTemplate.  

Applications requiring a batch framework can also take advantage of Spring Batch.  The appropriate Spring Bach reader/writers for Stardog are also included.
  
Future Spring project integration is likely, and we welcome contributions on Github.


![Stardog](http://stardog.com/_/img/sdog.png)   

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
2. Use Gradle with fileTree filter to Stardog lib folder
3. In your Spring application context, create a DataSourceFactoryBean as your datasource, and reference it in a SnarlTemplate bean
4. Inject your SnarlTemplate bean appropriately in your application.  It is thread safe.


## Development ##

This project is built with Gradle, and can be easily imported into SpringSource Tool Suite or any other Eclipse based IDE. 


1. Clone the git repository
2. Edit build.gradle to point to Stardog/lib folder OR set the STARDOG environment variable
3. Run gradle build
4. Pick up the latest jar in build/libs

All tests should pass.  If you do not wish to run Emma code coverage tool, remove it from the Gradle configuration

## NOTE ##

This framework is in continuous development, please check the [issues](https://github.com/clarkparsia/stardog-spring/issues) page. You're welcome to contribute.
