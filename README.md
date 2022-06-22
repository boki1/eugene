[![Actions Status](https://github.com/boki1/eugene/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/boki1/eugene/actions/workflows/build-and-test.yml)
[![MIT License](https://img.shields.io/apm/l/atomic-design-ui.svg?)](https://github.com/tterb/atomic-design-ui/blob/master/LICENSEs)

<p align="center">
  <img src="https://user-images.githubusercontent.com/36764968/169560274-bafcc010-04af-4cc4-af92-c8a103d9062b.png" />
</p>

[Home](https://boki1.github.io/eugene/)

<h2>
System for Data Storing and Retrieving Based on Large Graph Processing
</h2>

<h4>Goals</h4>

<p>
  This system is developed as a thesis project in TUES.
  Thesis defense presentation may be found in <a href="https://github.com/boki1/eugene-slides">another repo</a>.
</p>

### Features

- Client/server communication
- Storage system based on B-trees
- Support for bulk operations, concurrent access and dynamic entries on the data
- Compression capabilities

## Docs

- About storage engine: [link](https://drive.google.com/file/d/1zT56mOAl3wQGoWHtyldhNyxYcsKTWsuJ/view?usp=sharing)
- About server and compression: [link](https://drive.google.com/file/d/1Yq7Ax58-CievKgJf__unERXR0vCGH8Ps/view?usp=sharing)


## Building

- Clone the repo
`gh repo clone boki1/eugene.git`

- Build with tests
`make clean-test`

- Build examples
`make clean-example`

## Contributing
The current maintainers of the project are [Kristiyan Stoimenov](https://www.linkedin.com/in/kristiyan-stoimenov/) and [Stoyan Tinchev](https://www.linkedin.com/in/stoyan-tinchev-524949208/).

## License
MIT License
