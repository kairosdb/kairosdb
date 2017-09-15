import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChange} from '@angular/core';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap'
import * as _ from 'lodash';
import {PsViewProperty} from '../../model/ps';
import * as type from '../../utils/type'

// PS stands for Processing Stage

@Component({
    selector: 'kairos-ps-field',
    templateUrl: './generic-ps-field.component.html',
    styleUrls: ['./generic-ps-field.component.css']
})
export class PsFieldComponent implements OnChanges, OnInit {
    @Input()
    public psProperty: PsViewProperty;

    public fieldValue: any;
    public valueArray: any[];
    public suggestions: string[];

    @Output()
    public change = new EventEmitter<void>();

    @Input()
    public tagValuesForNames: {};

    public constructor() {
        this.psProperty = new PsViewProperty();
        this.valueArray = [];
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if (changes['psProperty'] || changes['tagValuesForNames']) {
            let isArray = type.isType(this.psProperty, 'array');

            this.fieldValue = type.getDefault(this.psProperty);

            if (isArray) {
                this.valueArray = (this.fieldValue) ? this.fieldValue.toString().split(',') : [];
                this.fieldValue = '';

                this.suggestions = [];
                this.suggestions = (this.psProperty.autocomplete === "tags") ? _.keys(this.tagValuesForNames) : this.suggestions;
            }

            this.psProperty.value = (isArray) ? this.valueArray : this.fieldValue;
            type.validate(this.psProperty, (type.isType(this.psProperty, 'array')) ? this.valueArray : this.fieldValue);
        }
    }

    ngOnInit() {
    }

    onPropertyInputChange() {
        if (!type.validate(this.psProperty, (type.isType(this.psProperty, 'array')) ? this.valueArray : this.fieldValue)) return;
        if (!type.isType(this.psProperty, 'array')) {
            this.psProperty.value = this.fieldValue;
            this.change.emit();
        }
    }

    onEnter() {
        if (type.isType(this.psProperty, 'array')) {
            let array = _.clone(this.valueArray)
            array.push(this.fieldValue)
            if (type.validate(this.psProperty, array)) {
                this.valueArray.push(this.fieldValue);
                this.psProperty.value = this.valueArray;
                this.fieldValue = '';
            }
            // console.log(this.valueArray)
            this.change.emit();
        }
    }

    removeSelectedTag(idx: number) {
        _.pullAt(this.valueArray, idx);
        this.psProperty.value = this.valueArray;
        this.change.emit();
    }

    isValidField(prop: PsViewProperty, _type: string): boolean {
        if (!prop) { return false }
        switch (_type.toLowerCase())
        {
            case 'textarea':
                return type.isType(prop, 'string') && prop.multiline
            case 'simple_array':
                return type.isType(prop, 'array') && !prop.autocomplete
            case 'typeahead':
                return type.isType(prop, 'array') && (prop.autocomplete !== undefined && prop.autocomplete !== "")
            case 'select':
                return type.isType(prop, 'enum')
            case 'checkbox':
                return type.isType(prop, 'boolean')
            case 'input':
                return !type.isType(prop, 'array', 'enum', 'boolean') && !prop.multiline
            case 'array':
                return type.isType(prop, 'array')
        }
        return false
    }
}
