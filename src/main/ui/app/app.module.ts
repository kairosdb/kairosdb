import {NgModule} from '@angular/core';
import {BrowserModule} from '@angular/platform-browser';
import {FormsModule} from '@angular/forms';
import {HttpModule} from '@angular/http';
import {AlertModule, ButtonsModule, TooltipModule, TypeaheadModule} from 'ng2-bootstrap/ng2-bootstrap';

import {AppRoutingModule} from './app-routing.module';
import {TimeRangeModule} from './time-range.module';

import {SharedModule} from './shared/shared.module';

import {DescriptorService} from './descriptors.service';
import {QueryService} from './query.service';

import {AppComponent} from './components/app.component';
import {LineChartComponent} from './components/chart/line-chart.component';
import {MetricEditorComponent} from './components/metric/metric-editor.component';
import {MetricListComponent} from './components/metric/metric-list.component';
import {QueryComponent} from './components/query/query.component';
import {QueryStatusComponent} from './components/query/query-status.component';
import {PsListComponent} from './components/ps/generic-ps-list.component';
import {PsEditorComponent} from './components/ps/generic-ps-editor.component';
import {PsFieldComponent} from './components/ps/generic-ps-field.component';
import {TagEditorComponent} from './components/tag/tag-editor.component';
import {TagListComponent} from './components/tag/tag-list.component';

@NgModule({
    imports:      [ BrowserModule, FormsModule, HttpModule, AppRoutingModule,
        TimeRangeModule, AlertModule.forRoot(), ButtonsModule.forRoot(), TypeaheadModule.forRoot(), TooltipModule.forRoot(), SharedModule ],
        declarations: [
            AppComponent, QueryComponent, MetricEditorComponent, MetricListComponent,
            TagEditorComponent, TagListComponent, PsListComponent, PsEditorComponent, PsFieldComponent,
            LineChartComponent, QueryStatusComponent
        ],
        bootstrap:    [ AppComponent ],
        providers:    [ QueryService, DescriptorService ]
    })
    export class AppModule { }
