import {PsViewProperty} from '../model/ps';
import * as validation from './validation';

export function isType(prop: PsViewProperty, ...types: string[]): boolean {
    if (!prop) { return false }
    for (var type of types) {
        if (prop.type.toLowerCase() === type.toLowerCase()) {
            return true;
        }
    }
    return false;
}

export function getDefault(property: PsViewProperty): any {
    if (property.defaultValue === undefined) {
        return '';
    }
    switch (property.type.toLowerCase()) {
        case 'enum':
            let defaultValue = ''

            if (property.options !== undefined &&
                (defaultValue = property.options.find(opt => opt.toLowerCase() === property.defaultValue.toString().toLowerCase())))
                return defaultValue;
            else
                return (property.options !== undefined && property.options.length > 0) ? property.options[0] : ''
        case 'array':
            return property.defaultValue.slice(1, -1);
        case 'boolean':
            return property.defaultValue == 'true';
    }
    return property.defaultValue;
}

export function validate(prop: PsViewProperty, value: any) {
    let res = true;
    let message = '';

    if (this.isType(prop, 'integer') && !validation.isInteger(value)) message = 'This field must be an integer value.';
    if (this.isType(prop, 'long') && !validation.isLong(value)) message = 'This field must be a long value.';
    if (this.isType(prop, 'double') && !validation.isDouble(value)) message = 'This field must be a double value.';

    prop.error = message;
    if (prop.error.length > 0) return false;

    if (!prop.validations) return true;

    let formatted_value = value;
    if (typeof value === 'string') formatted_value = `"${value}"`;
    else if (value instanceof Array) formatted_value = value.length > 0 ? `["${value.join('", "')}"]` : '[]';

    prop.validations.forEach(validation => {
        if (validation.type.toString().toLowerCase() !== 'js'.toLowerCase()) {
            prop.error = `Validation can't evaluate expression type [${validation.type.toString()}]`;
            return;
        }

        if (!res) return;

        try { res = eval(`((value) => ${validation.expression})(${formatted_value})`); }
        catch (e) {}

        if (!res) message = validation.message.toString();
    })

    prop.error = message;
    return res;
}
