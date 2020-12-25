package org.apache.beam.sdk.extensions.ml;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class DelegatingAtomicCoder<X, Y> extends AtomicCoder<X> {

    private final Coder<Y> delegate;

    protected DelegatingAtomicCoder(Coder<Y> delegate) {
        this.delegate = delegate;
    }

    @Override
    public final X decode(InputStream inStream)
            throws CoderException, IOException {
        return from(delegate.decode(inStream));
    }

    @Override
    public final void encode(X value, OutputStream outStream)
            throws CoderException, IOException {
        delegate.encode(to(value), outStream);
    }

    protected abstract X from(Y object) throws CoderException, IOException;

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        delegate.verifyDeterministic();
    }

    protected abstract Y to(X object) throws CoderException, IOException;
}
