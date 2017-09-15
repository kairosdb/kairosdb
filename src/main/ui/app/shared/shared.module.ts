import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormsModule} from '@angular/forms';
// Imports for loading & configuring the in-memory web api
import {TypeaheadModule} from 'ng2-bootstrap/ng2-bootstrap';
import {TypeaheadComponent} from './typeahead.component';

@NgModule({
  imports:      [ CommonModule, FormsModule, TypeaheadModule ],
  declarations: [ TypeaheadComponent],
  exports:      [ TypeaheadComponent ]
})
export class SharedModule { }
