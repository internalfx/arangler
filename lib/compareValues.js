
function spaceship (a, b) {
  // A simple three-way comparison ("spaceship operator") function.
  // Using JS built-in comparison logic, returns 0, -1 or 1,
  // depending on whenever values compare equal, less or greater.
  // Returns null if neither of comparisons succeed (non-comparable).
  if (a === b) {
    return 0
  } else if (a < b) {
    return -1
  } else if (a > b) {
    return 1
  } else {
    throw new Error(`Uncomparable keys! ${a} <=> ${b}`)
  }
}

function compareValues (a, b) {
  // Returns 0, -1 or 1 (see `spaceship` function) or null if cannot compare.
  //
  // A special value of Infinity is treated as greater than any other value.
  // However, please note that the behavior of comparing two Infinity values is undefined.

  if (a === Infinity) { return 1 } // Infinity > *
  if (b === Infinity) { return -1 } // * < Infinity

  return spaceship(a, b)
}

module.exports = compareValues
