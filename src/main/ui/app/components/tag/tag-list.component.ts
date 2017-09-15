import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChange} from '@angular/core';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap'
import {Subject} from 'rxjs/Subject';
import * as _ from 'lodash';
import {QueryService} from '../../query.service'

@Component({
    selector: 'kairos-tag-list',
    templateUrl: './tag-list.component.html'
})
export class TagListComponent implements OnChanges, OnInit {
    @Input()
    public parsedSelectedTagObject: {}; // downstream
    @Output()
    public selectedTagObjectChange = new EventEmitter<{}>();

    @Input()
    public tagValuesForNames: {};

    public selectedTagArray: {}[];

    public duplicatedTagNames: string[];

    @Output()
    public error = new EventEmitter<Array<{}>>();

    public constructor(private queryService: QueryService) {
        // initialize empty arrays for typeahead component
        this.selectedTagArray = [];
        this.duplicatedTagNames = [];
        this.tagValuesForNames = {};
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if (changes['parsedSelectedTagObject']) {
            this.selectedTagArray = _.map(_.keys(this.parsedSelectedTagObject), (key) => { return { name: key, values: _.cloneDeep(this.parsedSelectedTagObject[key]) } });
        }
    }

    ngOnInit() {
    }

    addNew() {
        this.selectedTagArray.push({ name: '', values: [] });
    }

    update() {
        let seenOnce = {};
        let seenTwice = {};
        this.selectedTagArray.forEach(function (value, index) {
            let name = value['name'];
            if (seenOnce[name]) {
                seenTwice[name] = true;
            }
            else {
                seenOnce[name] = true;
            }
        });
        this.error.emit(_.keys(seenTwice));
        if (_.isEmpty(seenTwice)) {
            this.selectedTagObjectChange.emit(this.selectedTagListToObject());
        }
    }

    merge() {
        let newSelectedTagObject = this.selectedTagListToObject();
        this.selectedTagObjectChange.emit(newSelectedTagObject);
        this.selectedTagArray = _.map(_.keys(newSelectedTagObject), (key) => { return { name: key, values: newSelectedTagObject[key] } });
        this.error.emit([]);
    }

    delete(idx: number){
        this.selectedTagArray.splice(idx,1);
        this.update();
    }

    private selectedTagListToObject(): {} {
        let mergedSelectedTags = {};
        this.selectedTagArray.forEach(function (value, index) {
            let name = value['name'];
            if (mergedSelectedTags[name]) {
                mergedSelectedTags[name] = _.concat(mergedSelectedTags[name],value['values']);
            }
            else {
                mergedSelectedTags[name] = value['values'];
            }
        });
        mergedSelectedTags = _.mapValues(mergedSelectedTags, _.uniq);
        return mergedSelectedTags;
    }

}
