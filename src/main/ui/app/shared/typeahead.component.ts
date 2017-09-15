import {Component, EventEmitter, Input, Output} from '@angular/core';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap';

@Component({
    selector: 'kairos-typeahead',
    templateUrl: './typeahead.component.html',
    styleUrls: [ './typeahead.component.css' ],
})
export class TypeaheadComponent {
    @Input()
    public value: string;
    @Output()
    public valueChange = new EventEmitter<string>();

    @Output()
    public typeaheadLoading = new EventEmitter<boolean>();
    @Output()
    public typeaheadNoResults = new EventEmitter<boolean>();
    @Output()
    public typeaheadOnSelect = new EventEmitter<TypeaheadMatch>();
    @Output()
    public blur = new EventEmitter<any>();

    @Input()
    public typeaheadSource: any;
    @Input()
    public typeaheadMinLength: number;
    @Input()
    public typeaheadWaitMs: number;
    @Input()
    public typeaheadOptionsLimit: number;
    @Input()
    public typeaheadOptionField: string;
    @Input()
    public typeaheadGroupField: string;
    @Input()
    public typeaheadAsync: boolean = null;
    @Input()
    public typeaheadLatinize: boolean = true;
    @Input()
    public typeaheadWordDelimiters: string = ' ';
    @Input()
    public typeaheadSingleWords: boolean = true;
    @Input()
    public typeaheadPhraseDelimiters: string = '\'"';
    @Input()
    public placeholder: string = '';
}
