Stardog-Spring
==========

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
_Current Version **2.1.3**_

This is [Spring Framework](http://springsource.org) integration for [Stardog RDF Database](http://stardog.com). These bindings
provide Spring aware beans to provide an analogous feature set to Spring's
jdbcTemplate.  To support enterprise applications, integration is done with different Spring projects. 

Current Stardog Spring support includes:

* stardog-spring : Core Spring framework
* stardog-spring-batch: Spring Batch support

These projects are available in Maven central under the 'com.complexible.stardog' group id.


![Stardog](http://docs.stardog.com/img/sd.png)


## Contributing ##

This framework is in continuous development, please check the [issues](https://github.com/complexible/stardog-spring/issues) page. You're welcome to contribute.

## Building 

The general build workflow for Stardog Spring is as follows:

1. Update the core stardog-spring folder, build and run `gradle install`.  
2. Update stardog-spring-batch
3. Once all are updated, and the according Stardog release is available, the projects are published to Maven Central


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
