import {
    Component,
    EventEmitter,
    Input,
    OnChanges,
    OnInit,
    Output,
    QueryList,
    SimpleChange,
    ViewChildren
} from '@angular/core';
import * as _ from 'lodash';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap'
import {QueryService} from '../../query.service'
import {PsBase, PsDescriptor} from '../../model/ps';
import {PsEditorComponent} from './generic-ps-editor.component'

// PS stands for Processing Stage

@Component({
    selector: 'kairos-ps-list',
    templateUrl: './generic-ps-list.component.html',
    styleUrls: [ './generic-ps-list.component.css' ]
})
export class PsListComponent implements OnChanges, OnInit {
    @Input()
    public parsedPsObjectList: PsBase[];

    public workingPsObjectList: PsBase[];

    public generatedPsObjectList: PsBase[];


    @Output()
    public psObjectListChange = new EventEmitter<PsBase[]>();

    public selectedPsIndex: number;

    @Input()
    public psDescriptor: PsDescriptor;

    @Input()
    public tagValuesForNames: {};

    @ViewChildren(PsEditorComponent) psEditorComponents: QueryList<PsEditorComponent>;

    public constructor(public queryService: QueryService) {
        this.generatedPsObjectList = [];
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        // if(changes['psDescriptor']){}
        if (changes['parsedPsObjectList']) {
            this.selectedPsIndex = undefined;
            this.workingPsObjectList = _.map<PsBase,PsBase>(this.parsedPsObjectList, property => property) || [];
            this.generatedPsObjectList = _.map<PsBase,PsBase>(this.parsedPsObjectList, property => property) || [];
        }
    }

    ngOnInit() {
    }

    addNew() {
        let defaultPsName: string = _.get<string>(this.psDescriptor,'properties[0].name');
        this.workingPsObjectList.push(new PsBase(defaultPsName));
        this.generatedPsObjectList.push(new PsBase(defaultPsName));
        this.psObjectListChange.emit(this.generatedPsObjectList);
        this.selectedPsIndex = this.workingPsObjectList.length - 1;
    }

    selectionChange(idx: number) {
        if (idx !== this.selectedPsIndex && idx < this.generatedPsObjectList.length) {
            this.selectedPsIndex = idx;
        }
        else {
            this.selectedPsIndex = undefined;
        }
    }

    deletePs(idx: number) {
        if (idx === this.selectedPsIndex && idx < this.generatedPsObjectList.length) {
            this.selectedPsIndex = undefined;
        }
        else if (this.selectedPsIndex && idx < this.selectedPsIndex) {
            this.selectedPsIndex--;
        }
        _.pullAt(this.generatedPsObjectList, idx);
        _.pullAt(this.workingPsObjectList, idx);
        this.psObjectListChange.emit(this.generatedPsObjectList);
    }

    onPsNameChange(idx, name) {
        let newPsObject = _.cloneDeep(this.generatedPsObjectList[idx]);
        newPsObject.name = name;
        this.psEditorComponents.toArray()[idx].psChanged(newPsObject);
        this.psEditorComponents.toArray()[idx].updatePsObject();
        this.psObjectListChange.emit(this.generatedPsObjectList);
    }

    onPsEdit(idx, newPsObject) {
        this.generatedPsObjectList[idx] = newPsObject;
        this.psObjectListChange.emit(this.generatedPsObjectList);
    }

}
