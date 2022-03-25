Contributing to Benthos
=======================

Joining Club Blob by contributing to the Benthos project is a selfless, boring and occasionally painful act. As such any contributors to this project will be treated with the respect and compassion that they deserve.

Please be dull, please be respecting of others and their efforts, please do not take criticism or rejection of your ideas personally.

## Reporting Bugs

If you find a bug then please let the project know by opening an issue after doing the following:

- Do a quick search of the existing issues to make sure the bug isn't already reported
- Try and make a minimal list of steps that can reliably reproduce the bug you are experiencing
- Collect as much information as you can to help identify what the issue is (project version, configuration files, etc)

## Suggesting Enhancements

Having even the most casual interest in Benthos gives you honorary membership of Club Blob, entitling you to give a reserved (and hypothetical) tickle of the projects' toes in order to steer it in the direction of your whim.

Please don't abuse this entitlement, the poor blobfish can only gobble so many features before it starts to droop beyond repair. Enhancements should roughly follow the general goals of Benthos and be:

- Common use cases
- Simple to understand
- Simple to monitor

You can help us out by doing the following before raising a new issue:

- Check that the feature hasn't been requested already by searching existing issues
- Try and reduce your enhancement into a single, concise and deliverable request, rather than a general idea
- Explain your own use cases as the basis of the request

## Adding Features

Pull requests are always welcome. However, before going through the trouble of implementing a change it's worth creating an issue. This allows us to discuss the changes and make sure they are a good fit for the project.

Please always make sure a pull request has been:

- Unit tested with `make test`
- Linted with `make lint`
- Formatted with `make fmt`

If your change impacts inputs, outputs or other connectors then try to test them with `make test-integration`. If the integration tests aren't working on your machine then don't panic, just mention it in your PR.

If your change has an impact on documentation then make sure it is generated with `make docs`. You can test out the documentation site locally by running `yarn && yarn start` in the `./website` directory.

### Adding New Components

The APIs for adding new components (inputs, outputs, processors, caches, etc) has recently been simplified. If you are planning to create a new component you should use the latest implementations within `./internal/impl` as inspiration.

### Plugins

The core components within Benthos (inputs, processors, conditions and outputs) are all easily pluggable. If you are interested in adding new components please raise a ticket and we can discuss whether it's a good fit for the project.

If not then it's still easy to build your own version of Benthos with custom components. For guidance take a look at [this example repo](https://github.com/benthosdev/benthos-plugin-example).
