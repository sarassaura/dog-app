type Breed = string;
type SubBreed = string;

export interface DogsResponse {
    message: Map<Breed, SubBreed[]>;
    status: string;
}

export interface DogImageResponse {
    message: string | string[];
    status: string;
}

export interface SubBreedsResponse {
    message: string[];
    status: string;
}
