import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChange} from '@angular/core';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap'
import {Subject} from 'rxjs/Subject';
import * as _ from 'lodash';
import {QueryService} from '../../query.service'

@Component({
    selector: 'kairos-tag-editor',
    templateUrl: './tag-editor.component.html',
    styleUrls: [ './tag-editor.component.css' ]
})
export class TagEditorComponent implements OnChanges, OnInit {

    @Input()
    public tagValuesForNames: {};
    @Input()
    public selectedTagValues: string[];
    @Input()
    public tagName: string;

    @Output()
    public selectedTagValuesChange = new EventEmitter<string[]>();
    @Output()
    public tagNameChange = new EventEmitter<string>();
    @Output()
    public delete = new EventEmitter<void>();

    public tagNames: string[];
    public unselectedTagValues: string[];
    public refreshingMetricNames: boolean;
    public tagValue: string;

    public constructor(private queryService: QueryService) {
        // initialize empty arrays for typeahead component
        this.tagNames = [];
        this.unselectedTagValues = [];
        this.selectedTagValues = [];
        this.tagValuesForNames = {};
    }


    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if (changes['tagValuesForNames']) {
            this.tagNames = _.keys(this.tagValuesForNames) || [];
        }
        if (this.tagValuesForNames[this.tagName]) {
            this.unselectedTagValues = _.difference(this.tagValuesForNames[this.tagName], this.selectedTagValues);
        }
    }

    ngOnInit() {
    }

    removeSelectedTag(item: string) {
        let index = this.selectedTagValues.indexOf(item);
        this.selectedTagValues.splice(index, 1);
        if (this.tagValuesForNames
            && this.tagValuesForNames[this.tagName]
            && _.includes(this.tagValuesForNames[this.tagName],item)
            && !_.includes(this.unselectedTagValues,item)) {
            this.unselectedTagValues.push(item);
        }
        this.selectedTagValuesChange.emit(this.selectedTagValues);
    }

    addSelectedTag(item: string) {
        this.selectedTagValues.push(item);
        let index = this.unselectedTagValues.indexOf(item);
        this.unselectedTagValues.splice(index, 1);
        this.selectedTagValuesChange.emit(this.selectedTagValues);
    }

    onTagNameUpdate() {
        if (this.tagValuesForNames && this.tagValuesForNames[this.tagName]) {
            this.unselectedTagValues = _.difference(this.tagValuesForNames[this.tagName], this.selectedTagValues);
        }
        else {
            this.unselectedTagValues = [];
        }
    }

}
