use std::any::{TypeId, type_name};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServiceId(TypeId);

impl ServiceId {
    pub fn of<T: ?Sized + 'static>() -> Self {
        Self(TypeId::of::<T>())
    }

    pub fn type_id(self) -> TypeId {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ServiceKey {
    pub id: ServiceId,
    pub name: &'static str,
}

impl ServiceKey {
    pub fn of<T: ?Sized + 'static>() -> Self {
        Self {
            id: ServiceId::of::<T>(),
            name: type_name::<T>(),
        }
    }
}
