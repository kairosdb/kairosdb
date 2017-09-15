import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {QueryComponent} from './components/query/query.component';

const routes: Routes = [
    { path: '',  component: QueryComponent }
];

@NgModule({
    imports: [ RouterModule.forRoot(routes) ],
    exports: [ RouterModule ]
})
export class AppRoutingModule {}
