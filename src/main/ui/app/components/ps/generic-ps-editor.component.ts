import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChange} from '@angular/core';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap'
import {Subject} from 'rxjs/Subject';
import * as _ from 'lodash';
import {PsBase, PsDescribedProperty, PsDescriptor, PsProcessor, PsViewProperty, toViewProperty} from '../../model/ps';
import * as type from '../../utils/type';

// PS stands for Processing Stage

@Component({
    selector: 'kairos-ps-editor',
    templateUrl: './generic-ps-editor.component.html',
    styleUrls: [ './generic-ps-editor.component.css' ]
})
export class PsEditorComponent implements OnChanges, OnInit {
    @Input()
    public psDescriptor: PsDescriptor;

    @Input()
    public psObject: PsBase;

    @Output()
    public psObjectChange = new EventEmitter<PsDescribedProperty>();

    @Input()
    public tagValuesForNames: {};

    public psName: string;

    public showPsInfo: boolean;

    public currentPsDescription: PsProcessor;
    public currentPsProperties: PsViewProperty[];

    public constructor() {
        this.psDescriptor = new PsDescriptor();
        this.currentPsProperties = [];
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        // called only when the change
        if (changes['psObject'] || changes['psDescriptor']) {
            // console.log('ngOnChanges');
            this.psChanged(this.psObject);
        }
    }

    public psChanged(psObject: PsBase) {
        if (!this.psDescriptor || !(this.psDescriptor.properties)) {
            this.currentPsProperties = [];
            return;
        }
        this.psName = psObject.name;
        this.currentPsDescription = _.find(this.psDescriptor.properties, description => description.name === psObject.name);
        let newPsProperties = new Array<PsViewProperty>();
        if (this.currentPsDescription && this.currentPsDescription.properties) {
            this.currentPsDescription.properties.forEach((propertyDescribed) => {
                let property:PsViewProperty = toViewProperty(propertyDescribed);
                if (type.isType(property, 'object')) {
                    propertyDescribed.properties.forEach((subPropertyDescribed) => {
                        let subProperty: PsViewProperty = toViewProperty(subPropertyDescribed);
                        subProperty.parent_name = property.name;
                        let currentVal = _.get(psObject, subProperty.name);
                        subProperty.active = currentVal !== undefined || !subProperty.optional;
                        subProperty.value = currentVal || type.getDefault(subProperty);
                        newPsProperties.push(subProperty);
                    });
                }
                else {
                    let currentVal = _.get(psObject, property.name);
                    property.active = currentVal !== undefined || !property.optional;
                    property.value = currentVal || type.getDefault(property);
                    newPsProperties.push(property);
                }
            });

        }
        this.currentPsProperties = newPsProperties;
    }

    ngOnInit() {
    }

    updatePsObject() {
        let localPsObject = new PsBase(this.psName);
        this.currentPsProperties.forEach(property => {
            let propertyName = property.parent_name ? property.parent_name+'.'+property.name : property.name;
            if (property.active) {
                _.set(localPsObject, propertyName, property.value);
            }
            else {
                _.unset(localPsObject, propertyName);
            }
        });
        this.psObjectChange.emit(localPsObject);
    }

    onPropertyNameClick(propButton, psProperty) {
        propButton.blur();
        psProperty.active = !psProperty.active;
        this.updatePsObject();
    }

    onPropertyInputChange(psProperty: {}) {
        this.updatePsObject();
    }


}
