# ADcare

Web Intelligence project - IG5 2018/2019 Polytech Montpellier
Analyse of data about internet users and prediction models of his possibility to click on our ad.

## Getting Started

### Requirements
ADcare uses **Scala** language:
* [Scala](https://www.scala-lang.org/) - Builder for Scala projects - (developed with v2.11.0)

This project runs thanks to the Scala interactive build tool **sbt**:
* [SBT](https://www.scala-sbt.org/) - Builder for Scala projects - (developed with v1.2.4)

### Launching
The ADcare program can be use in a various ways. You cna run `sbt "run help"` to get all the information below. 
**Main usage:**
`sbt run`
equivalent to `sbt "run .data/data-students.json logisticRegression predict"`. This will be predict the ad click according to the information in the file *./data/data-students.json* using the logisticRegression algorithm.


## Authors

* **Hugo FAZIO** - [HugoMeatBoy](https://github.com/HugoMeatBoy)
* **Kévin GIORDANI** - [PurpleBooth](https://github.com/Rifhice)
* **Hugo MAITRE** - [HmFlashy](https://github.com/HmFlashy)
* **Clément ROIG** - [Clm-Roig](https://github.com/Clm-Roig)
* **Loris ZIRAH** - [Loriszirah](https://github.com/Loriszirah)


## License
This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details