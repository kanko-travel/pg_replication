use rdkafka::error::KafkaError;

#[derive(Debug)]
pub enum ConsumerError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

pub trait ErrorExt {
    fn is_recoverable(&self) -> bool;
}

impl<E: ErrorExt + Into<anyhow::Error>> From<E> for ConsumerError {
    fn from(err: E) -> Self {
        if err.is_recoverable() {
            Self::Recoverable(err.into())
        } else {
            Self::Fatal(err.into())
        }
    }
}

impl ErrorExt for KafkaError {
    fn is_recoverable(&self) -> bool {
        use KafkaError::*;

        match self {
            ClientCreation(..) | ClientConfig(..) => false,
            _ => true,
        }
    }
}
