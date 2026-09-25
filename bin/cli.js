import yargs from 'yargs/yargs';
import methods from '../index.js';
const argv = yargs(process.argv.slice(2)).parse();
async function run() {
  if (typeof methods[argv._[0]] !== 'function') throw new Error(`${argv._[0]} is not a function`);
  const output = await methods[argv._[0]](argv);
  console.log(output);
}
run();
