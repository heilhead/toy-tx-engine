## Payment Transaction Engine

### Assumptions

The following was not clear from the task description, so I had to make assumptions:

- Transaction IDs are globally unique;
- Only 'deposit' transactions can be reversed;
- Negative balance is not allowed;

### Error Handling

All the data storage containers, as well as the engine, provide detailed errors, so it's possible to tell what exactly went wrong during transaction processing.

On the app level, the errors are printed to `stderr`, which shouldn't interfere with the automated testing and data serialization to `stdio`.

### Test Coverage

- All transaction types;
- All transaction errors;
- All balance operations;
- All balance operation errors;

Unit tests are nested into parent modules for simplicity.
