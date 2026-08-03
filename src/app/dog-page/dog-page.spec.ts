import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DogPage } from './dog-page';

describe('DogPage', () => {
  let component: DogPage;
  let fixture: ComponentFixture<DogPage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DogPage],
    }).compileComponents();

    fixture = TestBed.createComponent(DogPage);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
