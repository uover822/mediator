const { generatorNum } = require('unique-sequence');

// A string generator using 2 characters: 'a' and 'b':
const gen = generatorNum();
console.log(gen.next().value); // a
console.log(gen.next().value); // b
console.log(gen.next().value); // ba
console.log(gen.next().value); // bb
console.log(gen.next().value); // baa

// and so on...
