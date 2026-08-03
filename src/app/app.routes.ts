import { Routes } from '@angular/router';
import { Home } from './home/home';
import { DogPage } from './dog-page/dog-page';

export const routes: Routes = [
    {
        path: '',
        component: Home,
    },
    {
        path: 'raca/:breed',
        component: DogPage
    },
    {
        path: 'raca/:breed/:subBreed',
        component: DogPage
    }
];
