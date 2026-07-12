from app.domain.venues.models import CanonicalInstrument, VenueInstrumentMapping


class InstrumentMappingRegistry:
    def __init__(
        self,
        instruments: tuple[CanonicalInstrument, ...],
        mappings: tuple[VenueInstrumentMapping, ...],
    ) -> None:
        self.instruments = {item.instrument_id: item for item in instruments}
        self.mappings = {(item.venue, item.venue_symbol): item for item in mappings}
        if len(self.instruments) != len(instruments) or len(self.mappings) != len(mappings):
            raise ValueError("duplicate canonical instrument or venue mapping")
        for mapping in mappings:
            instrument = self.instruments.get(mapping.canonical_instrument_id)
            if instrument is None or not mapping.matches(instrument):
                raise ValueError(
                    f"inconsistent venue mapping: {mapping.venue}:{mapping.venue_symbol}"
                )

    def resolve(self, venue: str, venue_symbol: str) -> CanonicalInstrument:
        try:
            mapping = self.mappings[(venue, venue_symbol)]
        except KeyError as exc:
            raise KeyError(f"instrument mapping not found: {venue}:{venue_symbol}") from exc
        return self.instruments[mapping.canonical_instrument_id]

    def require_same_instrument(
        self, left: tuple[str, str], right: tuple[str, str]
    ) -> CanonicalInstrument:
        left_instrument = self.resolve(*left)
        right_instrument = self.resolve(*right)
        if left_instrument.instrument_id != right_instrument.instrument_id:
            raise ValueError("venue symbols do not represent the same canonical instrument")
        return left_instrument
