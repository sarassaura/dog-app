import { Component, inject, signal, ViewEncapsulation } from '@angular/core';
import { Title } from '@angular/platform-browser';
import {ActivatedRoute} from '@angular/router';
import { Dogs } from '../dogs';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
import { RouterModule } from '@angular/router';

@Component({
  selector: 'app-dog-page',
  imports: [RouterModule, MatProgressSpinnerModule],
  templateUrl: './dog-page.html',
  styleUrl: './dog-page.css',
  encapsulation: ViewEncapsulation.None
})
export class DogPage {
  title = inject(Title);
  dogBreed: string = '';
  dogSubBreed: string | undefined | null = '';
  dogFullName: string = '';
  dogSubBreeds = signal<string[]>([]);
  dogImage = signal<string>('');
  route: ActivatedRoute = inject(ActivatedRoute);
  dogsService: Dogs = inject(Dogs);

  ngOnInit() {
    this.route.paramMap.subscribe(params => {
      this.dogBreed = params.get('breed') ?? '';
      this.dogSubBreed = params.get('subBreed');

      this.dogBreed && this.loadData(this.dogBreed, this.dogSubBreed);
    });
  }

  loadData(breed: string, subBreed?: string | null) {
    this.dogFullName = breed;
    if (breed && subBreed) this.dogFullName = `${breed} ${subBreed}`;
    this.title.setTitle(this.dogFullName);

    this.dogsService.getBreed(breed, subBreed).then((res) => {
      this.dogImage.set(res.message as string);
    });

    this.dogsService.getSubBreeds(breed).then((res) => {
      this.dogSubBreeds.set(res.message as string[]);
    })
  }
}
