import * as _ from 'lodash';

// ps stands for Processing Stage

export class PsBase {
    name: string;
    label: string;

    constructor(name?: string) {
        this.name = name;
    }
}

export class PsDescriptor extends PsBase {
    properties: PsProcessor[];
}

export class PsProcessor extends PsBase {
    description: string;
    properties: PsDescribedProperty[];
}

export abstract class AbstractPsProperty extends PsBase {
    description?: string;
    type?: string;
    optional?: boolean;
    validations?: PsPropertyValidation[]; // type to be defined
    defaultValue?: any;
    options?: string[];
    autocomplete?: string;
    multiline?: boolean;
}

export class PsDescribedProperty extends AbstractPsProperty {
    properties?: PsDescribedProperty[];
}

export class PsViewProperty extends AbstractPsProperty {
    parent_name: string;
    active: boolean;
    value: any;
    error: string;
}

export class PsPropertyValidation {
    expression: string;
    type: string;
    message: string;
}

export function toViewProperty(psDescribedProperty: PsDescribedProperty): PsViewProperty {
    let viewProperty = new PsViewProperty(psDescribedProperty.name);
    viewProperty.label = psDescribedProperty.label;
    viewProperty.optional = psDescribedProperty.optional;
    viewProperty.type = psDescribedProperty.type;
    viewProperty.validations = _.cloneDeep(psDescribedProperty.validations);
    viewProperty.defaultValue = psDescribedProperty.defaultValue;
    viewProperty.description = psDescribedProperty.description;
    viewProperty.options = _.cloneDeep(psDescribedProperty.options);
    viewProperty.autocomplete = psDescribedProperty.autocomplete;
    viewProperty.multiline = psDescribedProperty.multiline;
    return viewProperty;
}
