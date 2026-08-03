import { Component, inject, signal, ViewEncapsulation } from '@angular/core';
import { Dogs } from '../dogs';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';
import {MatProgressBarModule} from '@angular/material/progress-bar';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import {MatTreeModule} from '@angular/material/tree';
import { RouterModule } from '@angular/router';
import { Title } from '@angular/platform-browser';
import {FormControl, FormsModule, ReactiveFormsModule} from '@angular/forms';
import {Observable} from 'rxjs';
import {map, startWith} from 'rxjs/operators';
import {AsyncPipe} from '@angular/common';
import {MatAutocompleteModule, MatAutocompleteSelectedEvent} from '@angular/material/autocomplete';
import {MatInputModule} from '@angular/material/input';
import {MatFormFieldModule} from '@angular/material/form-field';
import { Router } from '@angular/router';

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [RouterModule, MatCardModule, MatChipsModule, MatProgressBarModule, 
    MatIconModule, MatButtonModule, MatTreeModule, FormsModule, MatFormFieldModule,
    MatInputModule, MatAutocompleteModule, ReactiveFormsModule, AsyncPipe],
  templateUrl: './home.html',
  styleUrl: './home.css',
  encapsulation: ViewEncapsulation.None
})
export class Home {
  breeds = signal<{ name: string; subBreeds: string[] }[]>([]);
  dogsService: Dogs = inject(Dogs);
  title = inject(Title);
  router = inject(Router);

  myControl = new FormControl<string | { label: string; route: string[] }>('');
  options = signal<{ label: string; route: string[] }[]>([]);
  filteredOptions: Observable<{ label: string; route: string[] }[]>;

  constructor() {
    this.filteredOptions = this.myControl.valueChanges.pipe(
      startWith(''),
      map(value => {
        const filterValue =
          typeof value === 'string' ? value : value?.label ?? '';

        return this._filter(filterValue);
      }),
    );
  }

  async ngOnInit() {
    this.title.setTitle('Dogs');
    const res = await this.dogsService.getAllBreeds();

    this.breeds.set(
      Object.entries(res.message).map(([name, subBreeds]) => ({
        name,
        subBreeds,
      }))
    );
    let filterValues: { label: string; route: string[] }[] = [];
    this.breeds().forEach((one) => {
      one.subBreeds.forEach((sub: string) => {
        filterValues.push({
          label: `${one.name} ${sub}`,
          route: ['/raca', one.name, sub]
        });
      });
    })
    this.options.set(filterValues);
  }

  private _filter(value: string): { label: string; route: string[] }[] {
    const filterValue = value.toLowerCase();

    return this.options().filter(option => option.label.toLowerCase().includes(filterValue));
  }

  onOptionSelected(event: MatAutocompleteSelectedEvent) {
    this.router.navigate(event.option.value.route);
  }
}
