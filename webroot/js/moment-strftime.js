(function () {
  var moment, replacements;

  if (typeof require === "function") {
    moment = require('moment');
  } else {
    moment = this.moment;
  }

  replacements = {
    'a': 'ddd',
    'A': 'dddd',
    'b': 'MMM',
    'B': 'MMMM',
    'c': 'lll',
    'd': 'DD',
    '-d': 'D',
    'e': 'D',
    'F': 'YYYY-MM-DD',
    'H': 'HH',
    '-H': 'H',
    'I': 'hh',
    '-I': 'h',
    'j': 'DDDD',
    '-j': 'DDD',
    'k': 'H',
    'l': 'h',
    'm': 'MM',
    '-m': 'M',
    'M': 'mm',
    '-M': 'm',
    'p': 'A',
    'P': 'a',
    'S': 'ss',
    '-S': 's',
    'u': 'E',
    'w': 'd',
    'W': 'WW',
    'x': 'll',
    'X': 'LTS',
    'y': 'YY',
    'Y': 'YYYY',
    'z': 'ZZ',
    'Z': 'z',
    'f': 'SSS',
    '%': '%'
  };

  moment.fn.strftime = function (format) {
    var momentFormat, tokens;

    // Break up format string based on strftime tokens
    tokens = format.split(/(%\-?.)/);
    momentFormat = tokens.map(function (token) {
      // Replace strftime tokens with moment formats
      if (token[0] === '%' && replacements.hasOwnProperty(token.substr(1))) {
        return replacements[token.substr(1)];
      }
      // Escape non-token strings to avoid accidental formatting
      return token.length > 0 ? '[' + token + ']' : token;
    }).join('');

    return this.format(momentFormat);
  };

  if (typeof module !== "undefined" && module !== null) {
    module.exports = moment;
  } else {
    this.moment = moment;
  }
}).call(this);
