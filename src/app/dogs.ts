import { Service } from '@angular/core';
import { DogImageResponse, DogsResponse, SubBreedsResponse } from './dogs-response';

@Service()
export class Dogs {
    private readonly url = 'https://dog.ceo/api';

    async getAllBreeds(): Promise<DogsResponse> {
        const response = await fetch(`${this.url}/breeds/list/all`);

        if (!response.ok) {
            throw new Error('Erro ao buscar raças');
        }

        return response.json();
    }

    async getBreed(breed: string, subBreed: string | undefined | null): Promise<DogImageResponse> {
        let urlBreed = `${this.url}/breed/${breed}/images/random`;
        if (breed && subBreed) urlBreed = `${this.url}/breed/${breed}/${subBreed}/images/random`;

        const response = await fetch(urlBreed);

        if (!response.ok) {
            throw new Error(`Erro ao buscar raça ${breed} ${subBreed}`);
        }

        return response.json();
    }

    async getSubBreeds(breed: string): Promise<SubBreedsResponse> {
        const response = await fetch(`${this.url}/breed/${breed}/list`);

        if (!response.ok) {
            throw new Error('Erro ao buscar sub-raças');
        }

        return response.json();
    }
}
