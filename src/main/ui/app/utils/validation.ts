const INTEGER_REGEX = /^([-+]?[1-9][0-9]{0,9}|0)$/;
const LONG_REGEX = /^([-+]?[1-9][0-9]*|0)$/;
const DOUBLE_REGEX = /^([-+]?[1-9][0-9]*|0)?(\.[0-9]+)?([eE][-+]?[0-9]+)?$/;

export function isInteger(value:string): boolean{
    return value!==undefined && INTEGER_REGEX.test(value);
}

export function isLong(value:string): boolean{
    return value!==undefined && LONG_REGEX.test(value);
}

export function isDouble(value:string): boolean{
    return value!==undefined && DOUBLE_REGEX.test(value);
}
